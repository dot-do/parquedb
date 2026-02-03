/**
 * Tests for Comprehensive Observability Telemetry
 *
 * Tests the TelemetryCollector, write throughput, cache metrics,
 * event log growth, consistency lag, tracing, and structured logging.
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import {
  TelemetryCollector,
  createTelemetryCollector,
  getGlobalTelemetry,
  resetGlobalTelemetry,
  computeHistogramSummary,
  generateTraceId,
  generateSpanId,
  type TelemetryConfig,
  type WriteThroughputMetrics,
  type CacheMetrics,
  type EventLogMetrics,
  type ConsistencyLagMetrics,
  type TelemetrySnapshot,
} from '../../../src/observability/telemetry'

// =============================================================================
// TelemetryCollector Tests
// =============================================================================

describe('TelemetryCollector', () => {
  let telemetry: TelemetryCollector

  beforeEach(() => {
    resetGlobalTelemetry()
    telemetry = new TelemetryCollector({
      shardId: 'test-shard',
      environment: 'test',
      serviceName: 'parquedb-test',
    })
  })

  afterEach(() => {
    telemetry.stopPeriodicFlush()
    resetGlobalTelemetry()
  })

  // =========================================================================
  // Write Throughput Tests
  // =========================================================================

  describe('write throughput', () => {
    it('should record write operations', () => {
      telemetry.recordWrite('posts', 1024, 5.2)
      telemetry.recordWrite('posts', 2048, 3.1)
      telemetry.recordWrite('users', 512, 2.5)

      const metrics = telemetry.getWriteThroughput()

      expect(metrics.shardId).toBe('test-shard')
      expect(metrics.operationCount).toBe(3)
      expect(metrics.bytesWritten).toBe(1024 + 2048 + 512)
      expect(metrics.opsPerSecond).toBeGreaterThan(0)
      expect(metrics.bytesPerSecond).toBeGreaterThan(0)
    })

    it('should track per-namespace metrics', () => {
      telemetry.recordWrite('posts', 1024, 5.0)
      telemetry.recordWrite('posts', 2048, 3.0)
      telemetry.recordWrite('users', 512, 2.0)

      const metrics = telemetry.getWriteThroughput()

      expect(metrics.byNamespace.posts).toBeDefined()
      expect(metrics.byNamespace.posts.operationCount).toBe(2)
      expect(metrics.byNamespace.posts.bytesWritten).toBe(3072)

      expect(metrics.byNamespace.users).toBeDefined()
      expect(metrics.byNamespace.users.operationCount).toBe(1)
      expect(metrics.byNamespace.users.bytesWritten).toBe(512)
    })

    it('should compute latency histogram', () => {
      telemetry.recordWrite('posts', 100, 1.0)
      telemetry.recordWrite('posts', 100, 5.0)
      telemetry.recordWrite('posts', 100, 10.0)
      telemetry.recordWrite('posts', 100, 20.0)
      telemetry.recordWrite('posts', 100, 50.0)

      const metrics = telemetry.getWriteThroughput()

      expect(metrics.latency.count).toBe(5)
      expect(metrics.latency.min).toBe(1.0)
      expect(metrics.latency.max).toBe(50.0)
      expect(metrics.latency.avg).toBeCloseTo(17.2, 1)
      expect(metrics.latency.p50).toBeGreaterThanOrEqual(5.0)
      expect(metrics.latency.p99).toBeGreaterThanOrEqual(20.0)
    })

    it('should handle empty write metrics', () => {
      const metrics = telemetry.getWriteThroughput()

      expect(metrics.operationCount).toBe(0)
      expect(metrics.bytesWritten).toBe(0)
      expect(metrics.latency.count).toBe(0)
      expect(metrics.latency.avg).toBe(0)
    })

    it('should not record when disabled', () => {
      const disabled = new TelemetryCollector({ enabled: false })
      disabled.recordWrite('posts', 1024, 5.0)

      const metrics = disabled.getWriteThroughput()
      expect(metrics.operationCount).toBe(0)
    })
  })

  // =========================================================================
  // Cache Metrics Tests
  // =========================================================================

  describe('cache metrics', () => {
    it('should record cache hits and misses', () => {
      telemetry.recordCacheHit('query_cache')
      telemetry.recordCacheHit('query_cache')
      telemetry.recordCacheMiss('query_cache')

      const metrics = telemetry.getCacheMetrics('query_cache')

      expect(metrics.hits).toBe(2)
      expect(metrics.misses).toBe(1)
      expect(metrics.hitRatio).toBeCloseTo(2 / 3, 4)
    })

    it('should record cache evictions', () => {
      telemetry.recordCacheEviction('query_cache')
      telemetry.recordCacheEviction('query_cache')

      const metrics = telemetry.getCacheMetrics('query_cache')
      expect(metrics.evictions).toBe(2)
    })

    it('should update cache size', () => {
      telemetry.updateCacheSize('query_cache', 50, 100, 8192)

      const metrics = telemetry.getCacheMetrics('query_cache')
      expect(metrics.size).toBe(50)
      expect(metrics.maxSize).toBe(100)
      expect(metrics.utilization).toBeCloseTo(0.5, 4)
      expect(metrics.bytesStored).toBe(8192)
    })

    it('should get all cache metrics', () => {
      telemetry.recordCacheHit('cache_a')
      telemetry.recordCacheMiss('cache_b')

      const allMetrics = telemetry.getAllCacheMetrics()

      expect(Object.keys(allMetrics)).toHaveLength(2)
      expect(allMetrics.cache_a).toBeDefined()
      expect(allMetrics.cache_b).toBeDefined()
    })

    it('should handle zero denominator for hit ratio', () => {
      const metrics = telemetry.getCacheMetrics('empty_cache')
      expect(metrics.hitRatio).toBe(0)
    })
  })

  // =========================================================================
  // Event Log Growth Tests
  // =========================================================================

  describe('event log metrics', () => {
    it('should record event log writes', () => {
      telemetry.recordEventLogWrite(10, 2048, 'CREATE')
      telemetry.recordEventLogWrite(5, 1024, 'UPDATE')
      telemetry.recordEventLogWrite(2, 512, 'DELETE')

      const metrics = telemetry.getEventLogMetrics()

      expect(metrics.eventCount).toBe(17)
      expect(metrics.sizeBytes).toBe(3584)
      expect(metrics.byOperation.CREATE).toBe(10)
      expect(metrics.byOperation.UPDATE).toBe(5)
      expect(metrics.byOperation.DELETE).toBe(2)
    })

    it('should compute events per minute', () => {
      // Record events (they are within the last minute)
      telemetry.recordEventLogWrite(10, 1024, 'CREATE')
      telemetry.recordEventLogWrite(5, 512, 'UPDATE')

      const metrics = telemetry.getEventLogMetrics()
      expect(metrics.eventsPerMinute).toBe(15)
      expect(metrics.bytesPerMinute).toBe(1536)
    })

    it('should compute average event size', () => {
      telemetry.recordEventLogWrite(10, 5000, 'CREATE')

      const metrics = telemetry.getEventLogMetrics()
      expect(metrics.avgEventSizeBytes).toBeCloseTo(500, 0)
    })

    it('should update absolute event log size', () => {
      telemetry.updateEventLogSize(1_000_000, 5000)

      const metrics = telemetry.getEventLogMetrics()
      expect(metrics.sizeBytes).toBe(1_000_000)
      expect(metrics.eventCount).toBe(5000)
    })

    it('should track timestamps', () => {
      telemetry.recordEventLogWrite(1, 100, 'CREATE')

      const metrics = telemetry.getEventLogMetrics()
      expect(metrics.oldestEventTs).toBeGreaterThan(0)
      expect(metrics.newestEventTs).toBeGreaterThan(0)
    })

    it('should handle empty event log', () => {
      const metrics = telemetry.getEventLogMetrics()
      expect(metrics.eventCount).toBe(0)
      expect(metrics.eventsPerMinute).toBe(0)
      expect(metrics.oldestEventTs).toBeNull()
    })
  })

  // =========================================================================
  // Consistency Lag Tests
  // =========================================================================

  describe('consistency lag', () => {
    it('should record lag measurements', () => {
      telemetry.recordConsistencyLag('posts', 10)
      telemetry.recordConsistencyLag('posts', 20)
      telemetry.recordConsistencyLag('users', 30)

      const metrics = telemetry.getConsistencyLagMetrics()

      expect(metrics.measurementCount).toBe(3)
      expect(metrics.currentLagMs).toBe(30)
      expect(metrics.avgLagMs).toBe(20)
      expect(metrics.maxLagMs).toBe(30)
    })

    it('should track per-namespace lag', () => {
      telemetry.recordConsistencyLag('posts', 10)
      telemetry.recordConsistencyLag('posts', 20)
      telemetry.recordConsistencyLag('users', 50)

      const metrics = telemetry.getConsistencyLagMetrics()

      expect(metrics.byNamespace.posts.currentLagMs).toBe(20)
      expect(metrics.byNamespace.posts.avgLagMs).toBe(15)
      expect(metrics.byNamespace.users.currentLagMs).toBe(50)
    })

    it('should track stale reads', () => {
      telemetry.recordStaleRead()
      telemetry.recordStaleRead()

      const metrics = telemetry.getConsistencyLagMetrics()
      expect(metrics.staleReadCount).toBe(2)
    })

    it('should compute percentiles', () => {
      for (let i = 1; i <= 100; i++) {
        telemetry.recordConsistencyLag('test', i)
      }

      const metrics = telemetry.getConsistencyLagMetrics()
      expect(metrics.p95LagMs).toBeGreaterThanOrEqual(95)
      expect(metrics.p99LagMs).toBeGreaterThanOrEqual(99)
    })

    it('should handle empty lag data', () => {
      const metrics = telemetry.getConsistencyLagMetrics()
      expect(metrics.currentLagMs).toBe(0)
      expect(metrics.avgLagMs).toBe(0)
      expect(metrics.measurementCount).toBe(0)
    })
  })

  // =========================================================================
  // Distributed Tracing Tests
  // =========================================================================

  describe('tracing', () => {
    it('should start and end spans', () => {
      const span = telemetry.startSpan('query.execute', {
        'db.namespace': 'posts',
      })

      expect(span.traceId).toHaveLength(32)
      expect(span.spanId).toHaveLength(16)
      expect(span.operationName).toBe('query.execute')
      expect(span.status).toBe('unset')

      telemetry.endSpan(span.spanId, 'ok', {
        'db.rows_matched': 42,
      })

      const completed = telemetry.getCompletedSpans()
      expect(completed).toHaveLength(1)
      expect(completed[0].status).toBe('ok')
      expect(completed[0].durationMs).toBeGreaterThanOrEqual(0)
      expect(completed[0].attributes['db.rows_matched']).toBe(42)
    })

    it('should support parent spans', () => {
      const parent = telemetry.startSpan('query.execute')
      const child = telemetry.startSpan('storage.read', {}, parent.spanId)

      expect(child.parentSpanId).toBe(parent.spanId)
    })

    it('should add span events', () => {
      const span = telemetry.startSpan('query.execute')

      telemetry.addSpanEvent(span.spanId, 'predicate_pushdown', {
        rowGroupsSkipped: 5,
      })

      // Span is still active so check via activeSpans count
      expect(telemetry.getActiveSpanCount()).toBe(1)

      telemetry.endSpan(span.spanId)

      const completed = telemetry.getCompletedSpans()
      expect(completed[0].events).toHaveLength(1)
      expect(completed[0].events[0].name).toBe('predicate_pushdown')
    })

    it('should include service attributes', () => {
      const span = telemetry.startSpan('test.op')

      expect(span.attributes['service.name']).toBe('parquedb-test')
      expect(span.attributes['shard.id']).toBe('test-shard')

      telemetry.endSpan(span.spanId)
    })

    it('should track active span count', () => {
      const span1 = telemetry.startSpan('op1')
      const span2 = telemetry.startSpan('op2')

      expect(telemetry.getActiveSpanCount()).toBe(2)

      telemetry.endSpan(span1.spanId)
      expect(telemetry.getActiveSpanCount()).toBe(1)

      telemetry.endSpan(span2.spanId)
      expect(telemetry.getActiveSpanCount()).toBe(0)
    })

    it('should handle ending non-existent span gracefully', () => {
      // Should not throw
      telemetry.endSpan('non-existent-span-id')
      expect(telemetry.getCompletedSpans()).toHaveLength(0)
    })
  })

  // =========================================================================
  // Structured Logging Tests
  // =========================================================================

  describe('structured logging', () => {
    it('should emit structured log entries', () => {
      telemetry.emitLog('info', 'query_completed', 'query', {
        rowsMatched: 42,
      }, {
        namespace: 'posts',
        operation: 'find',
        durationMs: 15,
      })

      const logs = telemetry.getLogs()
      expect(logs).toHaveLength(1)
      expect(logs[0].level).toBe('info')
      expect(logs[0].message).toBe('query_completed')
      expect(logs[0].component).toBe('query')
      expect(logs[0].namespace).toBe('posts')
      expect(logs[0].operation).toBe('find')
      expect(logs[0].durationMs).toBe(15)
      expect(logs[0].fields.rowsMatched).toBe(42)
      expect(logs[0].fields.shardId).toBe('test-shard')
    })

    it('should include error details', () => {
      const err = new Error('test error')
      telemetry.emitLog('error', 'query_failed', 'query', {}, {
        error: err,
      })

      const logs = telemetry.getLogs(undefined, 'error')
      expect(logs).toHaveLength(1)
      expect(logs[0].error).toBeDefined()
      expect(logs[0].error!.name).toBe('Error')
      expect(logs[0].error!.message).toBe('test error')
    })

    it('should filter logs by level', () => {
      telemetry.emitLog('debug', 'msg1', 'comp')
      telemetry.emitLog('info', 'msg2', 'comp')
      telemetry.emitLog('error', 'msg3', 'comp')

      const errorLogs = telemetry.getLogs(undefined, 'error')
      expect(errorLogs).toHaveLength(1)
      expect(errorLogs[0].message).toBe('msg3')
    })

    it('should limit log count', () => {
      for (let i = 0; i < 10; i++) {
        telemetry.emitLog('info', `msg${i}`, 'comp')
      }

      const limited = telemetry.getLogs(3)
      expect(limited).toHaveLength(3)
    })

    it('should not emit when disabled', () => {
      const disabled = new TelemetryCollector({ enabled: false })
      disabled.emitLog('info', 'test', 'comp')

      const logs = disabled.getLogs()
      expect(logs).toHaveLength(0)
    })
  })

  // =========================================================================
  // Snapshot Tests
  // =========================================================================

  describe('snapshot', () => {
    it('should produce a complete telemetry snapshot', () => {
      telemetry.recordWrite('posts', 1024, 5.0)
      telemetry.recordCacheHit('query_cache')
      telemetry.recordCacheMiss('query_cache')
      telemetry.recordEventLogWrite(5, 512, 'CREATE')
      telemetry.recordConsistencyLag('posts', 15)

      const snapshot = telemetry.getSnapshot()

      expect(snapshot.timestamp).toBeGreaterThan(0)
      expect(snapshot.service.name).toBe('parquedb-test')
      expect(snapshot.service.environment).toBe('test')
      expect(snapshot.service.shardId).toBe('test-shard')

      expect(snapshot.writeThroughput.operationCount).toBe(1)
      expect(snapshot.caches.query_cache.hits).toBe(1)
      expect(snapshot.caches.query_cache.misses).toBe(1)
      expect(snapshot.eventLog.eventCount).toBe(5)
      expect(snapshot.consistencyLag.currentLagMs).toBe(15)
    })
  })

  // =========================================================================
  // Prometheus Export Tests
  // =========================================================================

  describe('prometheus export', () => {
    it('should export metrics in prometheus format', () => {
      telemetry.recordWrite('posts', 1024, 5.0)
      telemetry.recordCacheHit('query_cache')
      telemetry.recordConsistencyLag('posts', 10)

      const output = telemetry.exportPrometheus()

      expect(output).toContain('parquedb_write_ops_total')
      expect(output).toContain('parquedb_write_bytes_total')
      expect(output).toContain('parquedb_cache_hits_total')
      expect(output).toContain('parquedb_consistency_lag_ms')
      expect(output).toContain('parquedb_event_log_size_bytes')
      expect(output).toContain('shard="test-shard"')
      expect(output).toContain('environment="test"')
    })
  })

  // =========================================================================
  // Lifecycle Tests
  // =========================================================================

  describe('lifecycle', () => {
    it('should flush to registered callbacks', async () => {
      const callback = vi.fn()
      telemetry.onFlush(callback)

      telemetry.recordWrite('posts', 1024, 5.0)
      await telemetry.flush()

      expect(callback).toHaveBeenCalledTimes(1)
      const snapshot = callback.mock.calls[0][0] as TelemetrySnapshot
      expect(snapshot.writeThroughput.operationCount).toBe(1)
    })

    it('should unregister flush callbacks', async () => {
      const callback = vi.fn()
      const unregister = telemetry.onFlush(callback)

      unregister()

      await telemetry.flush()
      expect(callback).not.toHaveBeenCalled()
    })

    it('should reset all metrics', () => {
      telemetry.recordWrite('posts', 1024, 5.0)
      telemetry.recordCacheHit('cache')
      telemetry.recordEventLogWrite(5, 512, 'CREATE')
      telemetry.recordConsistencyLag('posts', 10)

      telemetry.reset()

      const snapshot = telemetry.getSnapshot()
      expect(snapshot.writeThroughput.operationCount).toBe(0)
      expect(Object.keys(snapshot.caches)).toHaveLength(0)
      expect(snapshot.eventLog.eventCount).toBe(0)
      expect(snapshot.consistencyLag.measurementCount).toBe(0)
    })

    it('should close cleanly', async () => {
      const callback = vi.fn()
      telemetry.onFlush(callback)
      telemetry.startPeriodicFlush()

      telemetry.recordWrite('posts', 1024, 5.0)

      await telemetry.close()

      // Should have flushed on close
      expect(callback).toHaveBeenCalled()
    })
  })
})

// =============================================================================
// Global Telemetry Tests
// =============================================================================

describe('global telemetry', () => {
  afterEach(() => {
    resetGlobalTelemetry()
  })

  it('should create a singleton instance', () => {
    const t1 = getGlobalTelemetry()
    const t2 = getGlobalTelemetry()
    expect(t1).toBe(t2)
  })

  it('should reset the global instance', () => {
    const t1 = getGlobalTelemetry()
    resetGlobalTelemetry()
    const t2 = getGlobalTelemetry()
    expect(t1).not.toBe(t2)
  })

  it('should accept config on first creation', () => {
    const t = getGlobalTelemetry({ shardId: 'custom-shard' })
    t.recordWrite('posts', 100, 1.0)
    const metrics = t.getWriteThroughput()
    expect(metrics.shardId).toBe('custom-shard')
  })
})

// =============================================================================
// Factory Function Tests
// =============================================================================

describe('createTelemetryCollector', () => {
  it('should create a new collector instance', () => {
    const t1 = createTelemetryCollector({ shardId: 'shard-1' })
    const t2 = createTelemetryCollector({ shardId: 'shard-2' })

    expect(t1).not.toBe(t2)

    t1.recordWrite('posts', 100, 1.0)
    expect(t1.getWriteThroughput().operationCount).toBe(1)
    expect(t2.getWriteThroughput().operationCount).toBe(0)
  })
})

// =============================================================================
// Utility Function Tests
// =============================================================================

describe('computeHistogramSummary', () => {
  it('should compute summary for values', () => {
    const summary = computeHistogramSummary([1, 2, 3, 4, 5])
    expect(summary.count).toBe(5)
    expect(summary.sum).toBe(15)
    expect(summary.min).toBe(1)
    expect(summary.max).toBe(5)
    expect(summary.avg).toBe(3)
    expect(summary.p50).toBe(3)
  })

  it('should handle empty array', () => {
    const summary = computeHistogramSummary([])
    expect(summary.count).toBe(0)
    expect(summary.avg).toBe(0)
    expect(summary.min).toBe(0)
  })

  it('should handle single value', () => {
    const summary = computeHistogramSummary([42])
    expect(summary.count).toBe(1)
    expect(summary.min).toBe(42)
    expect(summary.max).toBe(42)
    expect(summary.p50).toBe(42)
    expect(summary.p95).toBe(42)
    expect(summary.p99).toBe(42)
  })

  it('should compute percentiles correctly', () => {
    // 100 values from 1 to 100
    const values = Array.from({ length: 100 }, (_, i) => i + 1)
    const summary = computeHistogramSummary(values)

    expect(summary.p50).toBeCloseTo(50.5, 0)
    expect(summary.p95).toBeCloseTo(95.05, 0)
    expect(summary.p99).toBeCloseTo(99.01, 0)
  })
})

describe('generateTraceId', () => {
  it('should generate 32-character hex string', () => {
    const id = generateTraceId()
    expect(id).toHaveLength(32)
    expect(/^[0-9a-f]{32}$/.test(id)).toBe(true)
  })

  it('should generate unique IDs', () => {
    const ids = new Set<string>()
    for (let i = 0; i < 100; i++) {
      ids.add(generateTraceId())
    }
    expect(ids.size).toBe(100)
  })
})

describe('generateSpanId', () => {
  it('should generate 16-character hex string', () => {
    const id = generateSpanId()
    expect(id).toHaveLength(16)
    expect(/^[0-9a-f]{16}$/.test(id)).toBe(true)
  })
})

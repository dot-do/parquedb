/**
 * Comprehensive Observability Telemetry for ParqueDB
 *
 * Provides centralized telemetry collection for:
 * - Write throughput per DO shard
 * - Cache hit/miss ratios
 * - Event log growth rate
 * - Consistency lag metrics
 * - Structured logging with context
 * - Trace context propagation
 *
 * @module observability/telemetry
 */

import { logger } from '../utils/logger'

// =============================================================================
// Telemetry Configuration
// =============================================================================

/**
 * Configuration for the telemetry collector
 */
export interface TelemetryConfig {
  /** Enable telemetry collection (default: true) */
  enabled?: boolean
  /** Flush interval for accumulated metrics in ms (default: 10000) */
  flushIntervalMs?: number
  /** Maximum number of metric data points to retain per series (default: 500) */
  maxDataPoints?: number
  /** Shard identifier for DO-based metrics */
  shardId?: string
  /** Environment label (e.g., 'production', 'staging') */
  environment?: string
  /** Service name for tracing (default: 'parquedb') */
  serviceName?: string
  /** Custom labels applied to all metrics */
  labels?: Record<string, string>
}

/**
 * Default telemetry configuration
 */
export const DEFAULT_TELEMETRY_CONFIG: Required<TelemetryConfig> = {
  enabled: true,
  flushIntervalMs: 10_000,
  maxDataPoints: 500,
  shardId: 'default',
  environment: 'production',
  serviceName: 'parquedb',
  labels: {},
}

// =============================================================================
// Metric Types
// =============================================================================

/**
 * Supported metric types matching OpenTelemetry conventions
 */
export type MetricType = 'counter' | 'gauge' | 'histogram'

/**
 * A single metric data point with timestamp
 */
export interface MetricPoint {
  /** Metric name */
  name: string
  /** Metric type */
  type: MetricType
  /** Metric value */
  value: number
  /** Timestamp (ms since epoch) */
  timestamp: number
  /** Labels for dimensional filtering */
  labels: Record<string, string>
}

/**
 * Histogram summary computed from collected data
 */
export interface HistogramSummary {
  /** Number of observations */
  count: number
  /** Sum of all observations */
  sum: number
  /** Minimum observed value */
  min: number
  /** Maximum observed value */
  max: number
  /** Average (mean) */
  avg: number
  /** p50 percentile */
  p50: number
  /** p95 percentile */
  p95: number
  /** p99 percentile */
  p99: number
}

// =============================================================================
// Write Throughput Metrics
// =============================================================================

/**
 * Write throughput metrics per DO shard
 */
export interface WriteThroughputMetrics {
  /** Shard identifier */
  shardId: string
  /** Total write operations in the current window */
  operationCount: number
  /** Total bytes written in the current window */
  bytesWritten: number
  /** Operations per second (computed) */
  opsPerSecond: number
  /** Bytes per second (computed) */
  bytesPerSecond: number
  /** Write latency histogram summary */
  latency: HistogramSummary
  /** Window start time */
  windowStart: number
  /** Window end time */
  windowEnd: number
  /** Breakdown by namespace */
  byNamespace: Record<string, {
    operationCount: number
    bytesWritten: number
  }>
}

// =============================================================================
// Cache Metrics
// =============================================================================

/**
 * Cache hit/miss metrics
 */
export interface CacheMetrics {
  /** Cache identifier (e.g., 'query_cache', 'metadata_cache') */
  cacheId: string
  /** Total cache hits */
  hits: number
  /** Total cache misses */
  misses: number
  /** Hit ratio (0..1) */
  hitRatio: number
  /** Total evictions */
  evictions: number
  /** Current cache size (entries) */
  size: number
  /** Maximum cache capacity */
  maxSize: number
  /** Utilization (size / maxSize) */
  utilization: number
  /** Bytes stored (if available) */
  bytesStored?: number
}

// =============================================================================
// Event Log Metrics
// =============================================================================

/**
 * Event log growth rate metrics
 */
export interface EventLogMetrics {
  /** Current event log size in bytes */
  sizeBytes: number
  /** Total events in log */
  eventCount: number
  /** Events per minute (rolling average) */
  eventsPerMinute: number
  /** Bytes per minute (rolling average) */
  bytesPerMinute: number
  /** Average event size in bytes */
  avgEventSizeBytes: number
  /** Time of oldest unarchived event */
  oldestEventTs: number | null
  /** Time of newest event */
  newestEventTs: number | null
  /** Estimated time until threshold breach (ms) */
  estimatedTimeToThresholdMs: number | null
  /** Breakdown by operation type */
  byOperation: Record<string, number>
}

// =============================================================================
// Consistency Lag Metrics
// =============================================================================

/**
 * Consistency lag metrics between write and read path
 */
export interface ConsistencyLagMetrics {
  /** Current lag in milliseconds (difference between last write and last read propagation) */
  currentLagMs: number
  /** Average lag over the measurement window */
  avgLagMs: number
  /** Maximum lag observed */
  maxLagMs: number
  /** p95 lag */
  p95LagMs: number
  /** p99 lag */
  p99LagMs: number
  /** Number of measurements in the window */
  measurementCount: number
  /** Stale read count (reads returning outdated data) */
  staleReadCount: number
  /** Per-namespace lag */
  byNamespace: Record<string, {
    currentLagMs: number
    avgLagMs: number
  }>
}

// =============================================================================
// Trace Context
// =============================================================================

/**
 * Trace context for distributed tracing
 */
export interface TraceContext {
  /** Trace ID (128-bit hex string) */
  traceId: string
  /** Span ID (64-bit hex string) */
  spanId: string
  /** Parent span ID (if this is a child span) */
  parentSpanId?: string
  /** Operation name */
  operationName: string
  /** Start timestamp (ms since epoch) */
  startTime: number
  /** End timestamp (if span is complete) */
  endTime?: number
  /** Duration in milliseconds */
  durationMs?: number
  /** Span status */
  status: 'ok' | 'error' | 'unset'
  /** Span attributes (key-value pairs) */
  attributes: Record<string, string | number | boolean>
  /** Span events (timestamped annotations) */
  events: Array<{
    name: string
    timestamp: number
    attributes?: Record<string, string | number | boolean>
  }>
}

// =============================================================================
// Structured Log Entry
// =============================================================================

/**
 * Structured log entry for observability
 */
export interface StructuredLogEntry {
  /** Log level */
  level: 'debug' | 'info' | 'warn' | 'error'
  /** Log message */
  message: string
  /** Timestamp (ms since epoch) */
  timestamp: number
  /** Component generating the log */
  component: string
  /** Operation context */
  operation?: string
  /** Namespace (if applicable) */
  namespace?: string
  /** Trace context (if available) */
  traceId?: string
  /** Span context (if available) */
  spanId?: string
  /** Additional structured fields */
  fields: Record<string, unknown>
  /** Duration (if this is a completion log) */
  durationMs?: number
  /** Error details */
  error?: {
    name: string
    message: string
    stack?: string
  }
}

// =============================================================================
// Snapshot of All Telemetry
// =============================================================================

/**
 * Complete telemetry snapshot at a point in time
 */
export interface TelemetrySnapshot {
  /** Timestamp of the snapshot */
  timestamp: number
  /** Service identification */
  service: {
    name: string
    environment: string
    shardId: string
  }
  /** Write throughput metrics per shard */
  writeThroughput: WriteThroughputMetrics
  /** Cache metrics by cache ID */
  caches: Record<string, CacheMetrics>
  /** Event log growth metrics */
  eventLog: EventLogMetrics
  /** Consistency lag metrics */
  consistencyLag: ConsistencyLagMetrics
  /** Custom labels */
  labels: Record<string, string>
}

// =============================================================================
// Telemetry Collector Implementation
// =============================================================================

/**
 * Centralized telemetry collector for ParqueDB
 *
 * Collects metrics, traces, and structured logs from all subsystems.
 * Thread-safe for use in Workers (single-threaded per isolate).
 *
 * @example
 * ```typescript
 * const telemetry = new TelemetryCollector({ shardId: 'shard-1' })
 *
 * // Record write operation
 * telemetry.recordWrite('posts', 1024, 5.2)
 *
 * // Record cache access
 * telemetry.recordCacheHit('query_cache')
 * telemetry.recordCacheMiss('query_cache')
 *
 * // Record event log growth
 * telemetry.recordEventLogWrite(3, 512)
 *
 * // Record consistency lag
 * telemetry.recordConsistencyLag('posts', 45)
 *
 * // Get snapshot of all metrics
 * const snapshot = telemetry.getSnapshot()
 * ```
 */
export class TelemetryCollector {
  private config: Required<TelemetryConfig>

  // Write throughput tracking
  private writeOps: Array<{ ns: string; bytes: number; latencyMs: number; ts: number }> = []
  private writeWindowStart: number

  // Cache tracking
  private cacheStats = new Map<string, {
    hits: number
    misses: number
    evictions: number
    size: number
    maxSize: number
    bytesStored: number
  }>()

  // Event log tracking
  private eventLogOps: Array<{ count: number; bytes: number; ts: number; op: string }> = []
  private eventLogSizeBytes = 0
  private eventLogEventCount = 0

  // Consistency lag tracking
  private lagMeasurements: Array<{ ns: string; lagMs: number; ts: number }> = []
  private staleReadCount = 0

  // Trace storage
  private activeSpans = new Map<string, TraceContext>()
  private completedSpans: TraceContext[] = []

  // Structured log buffer
  private logBuffer: StructuredLogEntry[] = []

  // Flush callbacks
  private flushCallbacks: Array<(snapshot: TelemetrySnapshot) => void | Promise<void>> = []

  // Flush timer
  private flushTimer: ReturnType<typeof setInterval> | null = null

  constructor(config: TelemetryConfig = {}) {
    this.config = { ...DEFAULT_TELEMETRY_CONFIG, ...config }
    this.writeWindowStart = Date.now()
  }

  // =========================================================================
  // Write Throughput
  // =========================================================================

  /**
   * Record a write operation
   *
   * @param namespace - Collection namespace
   * @param bytesWritten - Number of bytes written
   * @param latencyMs - Write latency in milliseconds
   */
  recordWrite(namespace: string, bytesWritten: number, latencyMs: number): void {
    if (!this.config.enabled) return

    this.writeOps.push({
      ns: namespace,
      bytes: bytesWritten,
      latencyMs,
      ts: Date.now(),
    })

    // Trim old data points
    this.trimArray(this.writeOps)

    this.emitLog('debug', 'write_recorded', 'storage', {
      namespace,
      bytesWritten,
      latencyMs,
    })
  }

  /**
   * Get current write throughput metrics
   */
  getWriteThroughput(): WriteThroughputMetrics {
    const now = Date.now()
    const windowDurationMs = now - this.writeWindowStart
    const windowDurationSec = Math.max(windowDurationMs / 1000, 0.001)

    // Aggregate by namespace
    const byNamespace: Record<string, { operationCount: number; bytesWritten: number }> = {}
    let totalOps = 0
    let totalBytes = 0
    const latencies: number[] = []

    for (const op of this.writeOps) {
      totalOps++
      totalBytes += op.bytes
      latencies.push(op.latencyMs)

      if (!byNamespace[op.ns]) {
        byNamespace[op.ns] = { operationCount: 0, bytesWritten: 0 }
      }
      byNamespace[op.ns].operationCount++
      byNamespace[op.ns].bytesWritten += op.bytes
    }

    return {
      shardId: this.config.shardId,
      operationCount: totalOps,
      bytesWritten: totalBytes,
      opsPerSecond: totalOps / windowDurationSec,
      bytesPerSecond: totalBytes / windowDurationSec,
      latency: computeHistogramSummary(latencies),
      windowStart: this.writeWindowStart,
      windowEnd: now,
      byNamespace,
    }
  }

  // =========================================================================
  // Cache Metrics
  // =========================================================================

  /**
   * Record a cache hit
   *
   * @param cacheId - Cache identifier
   */
  recordCacheHit(cacheId: string): void {
    if (!this.config.enabled) return
    const stats = this.ensureCacheStats(cacheId)
    stats.hits++
  }

  /**
   * Record a cache miss
   *
   * @param cacheId - Cache identifier
   */
  recordCacheMiss(cacheId: string): void {
    if (!this.config.enabled) return
    const stats = this.ensureCacheStats(cacheId)
    stats.misses++
  }

  /**
   * Record a cache eviction
   *
   * @param cacheId - Cache identifier
   */
  recordCacheEviction(cacheId: string): void {
    if (!this.config.enabled) return
    const stats = this.ensureCacheStats(cacheId)
    stats.evictions++
  }

  /**
   * Update cache size metrics
   *
   * @param cacheId - Cache identifier
   * @param size - Current entry count
   * @param maxSize - Maximum capacity
   * @param bytesStored - Optional bytes stored
   */
  updateCacheSize(cacheId: string, size: number, maxSize: number, bytesStored?: number): void {
    if (!this.config.enabled) return
    const stats = this.ensureCacheStats(cacheId)
    stats.size = size
    stats.maxSize = maxSize
    if (bytesStored !== undefined) {
      stats.bytesStored = bytesStored
    }
  }

  /**
   * Get cache metrics for a specific cache
   */
  getCacheMetrics(cacheId: string): CacheMetrics {
    const stats = this.ensureCacheStats(cacheId)
    const total = stats.hits + stats.misses
    return {
      cacheId,
      hits: stats.hits,
      misses: stats.misses,
      hitRatio: total > 0 ? stats.hits / total : 0,
      evictions: stats.evictions,
      size: stats.size,
      maxSize: stats.maxSize,
      utilization: stats.maxSize > 0 ? stats.size / stats.maxSize : 0,
      bytesStored: stats.bytesStored || undefined,
    }
  }

  /**
   * Get cache metrics for all tracked caches
   */
  getAllCacheMetrics(): Record<string, CacheMetrics> {
    const result: Record<string, CacheMetrics> = {}
    for (const cacheId of this.cacheStats.keys()) {
      result[cacheId] = this.getCacheMetrics(cacheId)
    }
    return result
  }

  // =========================================================================
  // Event Log Growth
  // =========================================================================

  /**
   * Record event log write activity
   *
   * @param eventCount - Number of events written
   * @param bytesWritten - Bytes written
   * @param operation - Event operation type (CREATE, UPDATE, DELETE)
   */
  recordEventLogWrite(eventCount: number, bytesWritten: number, operation: string = 'UNKNOWN'): void {
    if (!this.config.enabled) return

    this.eventLogOps.push({
      count: eventCount,
      bytes: bytesWritten,
      ts: Date.now(),
      op: operation,
    })

    this.eventLogSizeBytes += bytesWritten
    this.eventLogEventCount += eventCount

    // Trim old data points
    this.trimArray(this.eventLogOps)
  }

  /**
   * Update absolute event log size (from storage scan)
   *
   * @param sizeBytes - Total size in bytes
   * @param eventCount - Total event count
   */
  updateEventLogSize(sizeBytes: number, eventCount: number): void {
    if (!this.config.enabled) return
    this.eventLogSizeBytes = sizeBytes
    this.eventLogEventCount = eventCount
  }

  /**
   * Get event log growth metrics
   */
  getEventLogMetrics(): EventLogMetrics {
    const now = Date.now()
    const oneMinuteAgo = now - 60_000

    // Calculate rolling rates
    const recentOps = this.eventLogOps.filter(op => op.ts >= oneMinuteAgo)
    const eventsPerMinute = recentOps.reduce((sum, op) => sum + op.count, 0)
    const bytesPerMinute = recentOps.reduce((sum, op) => sum + op.bytes, 0)

    // Count by operation type
    const byOperation: Record<string, number> = {}
    for (const op of this.eventLogOps) {
      byOperation[op.op] = (byOperation[op.op] || 0) + op.count
    }

    // Calculate timestamps
    let oldestTs: number | null = null
    let newestTs: number | null = null
    if (this.eventLogOps.length > 0) {
      oldestTs = this.eventLogOps[0].ts
      newestTs = this.eventLogOps[this.eventLogOps.length - 1].ts
    }

    // Estimate avg event size
    const avgEventSize = this.eventLogEventCount > 0
      ? this.eventLogSizeBytes / this.eventLogEventCount
      : 0

    return {
      sizeBytes: this.eventLogSizeBytes,
      eventCount: this.eventLogEventCount,
      eventsPerMinute,
      bytesPerMinute,
      avgEventSizeBytes: avgEventSize,
      oldestEventTs: oldestTs,
      newestEventTs: newestTs,
      estimatedTimeToThresholdMs: null, // Consumer can compute based on thresholds
      byOperation,
    }
  }

  // =========================================================================
  // Consistency Lag
  // =========================================================================

  /**
   * Record a consistency lag measurement
   *
   * @param namespace - Collection namespace
   * @param lagMs - Observed lag in milliseconds
   */
  recordConsistencyLag(namespace: string, lagMs: number): void {
    if (!this.config.enabled) return

    this.lagMeasurements.push({
      ns: namespace,
      lagMs,
      ts: Date.now(),
    })

    // Trim old data points
    this.trimArray(this.lagMeasurements)

    // Log if lag is high
    if (lagMs > 1000) {
      this.emitLog('warn', 'high_consistency_lag', 'sync', {
        namespace,
        lagMs,
      })
    }
  }

  /**
   * Record a stale read (read returned outdated data)
   */
  recordStaleRead(): void {
    if (!this.config.enabled) return
    this.staleReadCount++
  }

  /**
   * Get consistency lag metrics
   */
  getConsistencyLagMetrics(): ConsistencyLagMetrics {
    const allLags = this.lagMeasurements.map(m => m.lagMs)

    // Per-namespace aggregation
    const byNamespace: Record<string, { currentLagMs: number; avgLagMs: number }> = {}
    const nsMeasurements = new Map<string, number[]>()

    for (const m of this.lagMeasurements) {
      if (!nsMeasurements.has(m.ns)) {
        nsMeasurements.set(m.ns, [])
      }
      nsMeasurements.get(m.ns)!.push(m.lagMs)
    }

    for (const [ns, lags] of nsMeasurements) {
      byNamespace[ns] = {
        currentLagMs: lags[lags.length - 1] ?? 0,
        avgLagMs: lags.length > 0 ? lags.reduce((a, b) => a + b, 0) / lags.length : 0,
      }
    }

    const summary = computeHistogramSummary(allLags)

    return {
      currentLagMs: allLags.length > 0 ? allLags[allLags.length - 1] : 0,
      avgLagMs: summary.avg,
      maxLagMs: summary.max,
      p95LagMs: summary.p95,
      p99LagMs: summary.p99,
      measurementCount: allLags.length,
      staleReadCount: this.staleReadCount,
      byNamespace,
    }
  }

  // =========================================================================
  // Distributed Tracing
  // =========================================================================

  /**
   * Start a new trace span
   *
   * @param operationName - Name of the operation being traced
   * @param attributes - Span attributes
   * @param parentSpanId - Optional parent span ID for nested spans
   * @returns Trace context for the new span
   */
  startSpan(
    operationName: string,
    attributes: Record<string, string | number | boolean> = {},
    parentSpanId?: string
  ): TraceContext {
    const span: TraceContext = {
      traceId: generateTraceId(),
      spanId: generateSpanId(),
      parentSpanId,
      operationName,
      startTime: Date.now(),
      status: 'unset',
      attributes: {
        ...attributes,
        'service.name': this.config.serviceName,
        'shard.id': this.config.shardId,
      },
      events: [],
    }

    this.activeSpans.set(span.spanId, span)
    return span
  }

  /**
   * End a trace span
   *
   * @param spanId - ID of the span to end
   * @param status - Final status of the span
   * @param attributes - Additional attributes to add
   */
  endSpan(
    spanId: string,
    status: 'ok' | 'error' = 'ok',
    attributes?: Record<string, string | number | boolean>
  ): void {
    const span = this.activeSpans.get(spanId)
    if (!span) return

    span.endTime = Date.now()
    span.durationMs = span.endTime - span.startTime
    span.status = status

    if (attributes) {
      Object.assign(span.attributes, attributes)
    }

    this.activeSpans.delete(spanId)
    this.completedSpans.push(span)

    // Trim completed spans
    if (this.completedSpans.length > this.config.maxDataPoints) {
      this.completedSpans = this.completedSpans.slice(-this.config.maxDataPoints)
    }
  }

  /**
   * Add an event to an active span
   *
   * @param spanId - ID of the span
   * @param name - Event name
   * @param attributes - Event attributes
   */
  addSpanEvent(
    spanId: string,
    name: string,
    attributes?: Record<string, string | number | boolean>
  ): void {
    const span = this.activeSpans.get(spanId)
    if (!span) return

    span.events.push({
      name,
      timestamp: Date.now(),
      attributes,
    })
  }

  /**
   * Get all completed spans (for export)
   */
  getCompletedSpans(): TraceContext[] {
    return [...this.completedSpans]
  }

  /**
   * Get active span count
   */
  getActiveSpanCount(): number {
    return this.activeSpans.size
  }

  // =========================================================================
  // Structured Logging
  // =========================================================================

  /**
   * Emit a structured log entry
   */
  emitLog(
    level: StructuredLogEntry['level'],
    message: string,
    component: string,
    fields: Record<string, unknown> = {},
    options?: {
      operation?: string
      namespace?: string
      traceId?: string
      spanId?: string
      durationMs?: number
      error?: Error
    }
  ): void {
    if (!this.config.enabled) return

    const entry: StructuredLogEntry = {
      level,
      message,
      timestamp: Date.now(),
      component,
      operation: options?.operation,
      namespace: options?.namespace,
      traceId: options?.traceId,
      spanId: options?.spanId,
      fields: {
        ...fields,
        shardId: this.config.shardId,
        environment: this.config.environment,
      },
      durationMs: options?.durationMs,
    }

    if (options?.error) {
      entry.error = {
        name: options.error.name,
        message: options.error.message,
        stack: options.error.stack,
      }
    }

    this.logBuffer.push(entry)

    // Trim log buffer
    if (this.logBuffer.length > this.config.maxDataPoints * 2) {
      this.logBuffer = this.logBuffer.slice(-this.config.maxDataPoints)
    }

    // Forward to logger
    switch (level) {
      case 'debug':
        logger.debug(`[${component}] ${message}`, fields)
        break
      case 'info':
        logger.info(`[${component}] ${message}`, fields)
        break
      case 'warn':
        logger.warn(`[${component}] ${message}`, fields)
        break
      case 'error':
        logger.error(`[${component}] ${message}`, options?.error, fields)
        break
    }
  }

  /**
   * Get buffered log entries
   *
   * @param limit - Maximum entries to return
   * @param level - Filter by log level
   */
  getLogs(limit?: number, level?: StructuredLogEntry['level']): StructuredLogEntry[] {
    let logs = this.logBuffer
    if (level) {
      logs = logs.filter(l => l.level === level)
    }
    if (limit) {
      logs = logs.slice(-limit)
    }
    return [...logs]
  }

  // =========================================================================
  // Snapshot & Export
  // =========================================================================

  /**
   * Get a complete telemetry snapshot
   */
  getSnapshot(): TelemetrySnapshot {
    return {
      timestamp: Date.now(),
      service: {
        name: this.config.serviceName,
        environment: this.config.environment,
        shardId: this.config.shardId,
      },
      writeThroughput: this.getWriteThroughput(),
      caches: this.getAllCacheMetrics(),
      eventLog: this.getEventLogMetrics(),
      consistencyLag: this.getConsistencyLagMetrics(),
      labels: { ...this.config.labels },
    }
  }

  /**
   * Export metrics in Prometheus text format
   */
  exportPrometheus(): string {
    const lines: string[] = []
    const timestamp = Date.now()
    const labels = this.formatPrometheusLabels({
      shard: this.config.shardId,
      environment: this.config.environment,
      ...this.config.labels,
    })

    // Write throughput metrics
    const wt = this.getWriteThroughput()
    lines.push('# HELP parquedb_write_ops_total Total write operations')
    lines.push('# TYPE parquedb_write_ops_total counter')
    lines.push(`parquedb_write_ops_total{${labels}} ${wt.operationCount} ${timestamp}`)

    lines.push('# HELP parquedb_write_bytes_total Total bytes written')
    lines.push('# TYPE parquedb_write_bytes_total counter')
    lines.push(`parquedb_write_bytes_total{${labels}} ${wt.bytesWritten} ${timestamp}`)

    lines.push('# HELP parquedb_write_ops_per_second Write operations per second')
    lines.push('# TYPE parquedb_write_ops_per_second gauge')
    lines.push(`parquedb_write_ops_per_second{${labels}} ${wt.opsPerSecond.toFixed(2)} ${timestamp}`)

    lines.push('# HELP parquedb_write_latency_ms Write latency in milliseconds')
    lines.push('# TYPE parquedb_write_latency_ms summary')
    lines.push(`parquedb_write_latency_ms{${labels},quantile="0.5"} ${wt.latency.p50.toFixed(2)} ${timestamp}`)
    lines.push(`parquedb_write_latency_ms{${labels},quantile="0.95"} ${wt.latency.p95.toFixed(2)} ${timestamp}`)
    lines.push(`parquedb_write_latency_ms{${labels},quantile="0.99"} ${wt.latency.p99.toFixed(2)} ${timestamp}`)
    lines.push(`parquedb_write_latency_ms_sum{${labels}} ${wt.latency.sum.toFixed(2)} ${timestamp}`)
    lines.push(`parquedb_write_latency_ms_count{${labels}} ${wt.latency.count} ${timestamp}`)

    // Per-namespace write throughput
    for (const [ns, nsMetrics] of Object.entries(wt.byNamespace)) {
      lines.push(`parquedb_write_ops_total{${labels},namespace="${ns}"} ${nsMetrics.operationCount} ${timestamp}`)
      lines.push(`parquedb_write_bytes_total{${labels},namespace="${ns}"} ${nsMetrics.bytesWritten} ${timestamp}`)
    }

    // Cache metrics
    for (const [cacheId, cm] of Object.entries(this.getAllCacheMetrics())) {
      const cacheLabels = `${labels},cache="${cacheId}"`
      lines.push(`parquedb_cache_hits_total{${cacheLabels}} ${cm.hits} ${timestamp}`)
      lines.push(`parquedb_cache_misses_total{${cacheLabels}} ${cm.misses} ${timestamp}`)
      lines.push(`parquedb_cache_hit_ratio{${cacheLabels}} ${cm.hitRatio.toFixed(4)} ${timestamp}`)
      lines.push(`parquedb_cache_evictions_total{${cacheLabels}} ${cm.evictions} ${timestamp}`)
      lines.push(`parquedb_cache_size{${cacheLabels}} ${cm.size} ${timestamp}`)
      lines.push(`parquedb_cache_utilization{${cacheLabels}} ${cm.utilization.toFixed(4)} ${timestamp}`)
    }

    // Event log metrics
    const el = this.getEventLogMetrics()
    lines.push('# HELP parquedb_event_log_size_bytes Event log size in bytes')
    lines.push('# TYPE parquedb_event_log_size_bytes gauge')
    lines.push(`parquedb_event_log_size_bytes{${labels}} ${el.sizeBytes} ${timestamp}`)

    lines.push('# HELP parquedb_event_log_count Total events in log')
    lines.push('# TYPE parquedb_event_log_count gauge')
    lines.push(`parquedb_event_log_count{${labels}} ${el.eventCount} ${timestamp}`)

    lines.push('# HELP parquedb_event_log_events_per_minute Events written per minute')
    lines.push('# TYPE parquedb_event_log_events_per_minute gauge')
    lines.push(`parquedb_event_log_events_per_minute{${labels}} ${el.eventsPerMinute} ${timestamp}`)

    // Consistency lag metrics
    const cl = this.getConsistencyLagMetrics()
    lines.push('# HELP parquedb_consistency_lag_ms Consistency lag in milliseconds')
    lines.push('# TYPE parquedb_consistency_lag_ms gauge')
    lines.push(`parquedb_consistency_lag_ms{${labels}} ${cl.currentLagMs} ${timestamp}`)

    lines.push('# HELP parquedb_consistency_lag_avg_ms Average consistency lag')
    lines.push('# TYPE parquedb_consistency_lag_avg_ms gauge')
    lines.push(`parquedb_consistency_lag_avg_ms{${labels}} ${cl.avgLagMs.toFixed(2)} ${timestamp}`)

    lines.push('# HELP parquedb_stale_reads_total Total stale reads observed')
    lines.push('# TYPE parquedb_stale_reads_total counter')
    lines.push(`parquedb_stale_reads_total{${labels}} ${cl.staleReadCount} ${timestamp}`)

    return lines.join('\n')
  }

  // =========================================================================
  // Lifecycle
  // =========================================================================

  /**
   * Register a callback to receive periodic metric snapshots
   */
  onFlush(callback: (snapshot: TelemetrySnapshot) => void | Promise<void>): () => void {
    this.flushCallbacks.push(callback)
    return () => {
      const idx = this.flushCallbacks.indexOf(callback)
      if (idx !== -1) {
        this.flushCallbacks.splice(idx, 1)
      }
    }
  }

  /**
   * Start periodic flushing of metrics
   */
  startPeriodicFlush(): void {
    if (this.flushTimer) return

    this.flushTimer = setInterval(() => {
      this.flush().catch(err => {
        logger.error('[telemetry] Periodic flush failed:', err)
      })
    }, this.config.flushIntervalMs)
  }

  /**
   * Stop periodic flushing
   */
  stopPeriodicFlush(): void {
    if (this.flushTimer) {
      clearInterval(this.flushTimer)
      this.flushTimer = null
    }
  }

  /**
   * Flush metrics to all registered callbacks
   */
  async flush(): Promise<void> {
    if (this.flushCallbacks.length === 0) return

    const snapshot = this.getSnapshot()
    await Promise.all(
      this.flushCallbacks.map(cb => cb(snapshot))
    )
  }

  /**
   * Reset all collected metrics
   */
  reset(): void {
    this.writeOps = []
    this.writeWindowStart = Date.now()
    this.cacheStats.clear()
    this.eventLogOps = []
    this.eventLogSizeBytes = 0
    this.eventLogEventCount = 0
    this.lagMeasurements = []
    this.staleReadCount = 0
    this.activeSpans.clear()
    this.completedSpans = []
    this.logBuffer = []
  }

  /**
   * Close the collector and release resources
   */
  async close(): Promise<void> {
    this.stopPeriodicFlush()
    await this.flush()
    this.reset()
  }

  // =========================================================================
  // Private Helpers
  // =========================================================================

  private ensureCacheStats(cacheId: string) {
    let stats = this.cacheStats.get(cacheId)
    if (!stats) {
      stats = { hits: 0, misses: 0, evictions: 0, size: 0, maxSize: 0, bytesStored: 0 }
      this.cacheStats.set(cacheId, stats)
    }
    return stats
  }

  private trimArray<T>(arr: T[]): void {
    if (arr.length > this.config.maxDataPoints) {
      arr.splice(0, arr.length - this.config.maxDataPoints)
    }
  }

  private formatPrometheusLabels(labels: Record<string, string>): string {
    return Object.entries(labels)
      .map(([k, v]) => `${k}="${v}"`)
      .join(',')
  }
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Compute histogram summary from an array of values
 */
export function computeHistogramSummary(values: number[]): HistogramSummary {
  if (values.length === 0) {
    return { count: 0, sum: 0, min: 0, max: 0, avg: 0, p50: 0, p95: 0, p99: 0 }
  }

  const sorted = [...values].sort((a, b) => a - b)
  const sum = sorted.reduce((a, b) => a + b, 0)

  return {
    count: sorted.length,
    sum,
    min: sorted[0],
    max: sorted[sorted.length - 1],
    avg: sum / sorted.length,
    p50: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    p99: percentile(sorted, 99),
  }
}

/**
 * Compute percentile from sorted array
 */
function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0
  if (sorted.length === 1) return sorted[0]

  const index = (p / 100) * (sorted.length - 1)
  const lower = Math.floor(index)
  const upper = Math.ceil(index)

  if (lower === upper) return sorted[lower]

  const fraction = index - lower
  return sorted[lower] + (sorted[upper] - sorted[lower]) * fraction
}

/**
 * Generate a 128-bit trace ID as hex string
 */
export function generateTraceId(): string {
  const hex = () => Math.random().toString(16).substring(2, 10).padStart(8, '0')
  return `${hex()}${hex()}${hex()}${hex()}`
}

/**
 * Generate a 64-bit span ID as hex string
 */
export function generateSpanId(): string {
  const hex = () => Math.random().toString(16).substring(2, 10).padStart(8, '0')
  return `${hex()}${hex()}`
}

// =============================================================================
// Global Telemetry Instance
// =============================================================================

/** Global telemetry collector instance */
let globalTelemetry: TelemetryCollector | null = null

/**
 * Get or create the global telemetry collector
 */
export function getGlobalTelemetry(config?: TelemetryConfig): TelemetryCollector {
  if (!globalTelemetry) {
    globalTelemetry = new TelemetryCollector(config)
  }
  return globalTelemetry
}

/**
 * Reset the global telemetry collector (useful for testing)
 */
export function resetGlobalTelemetry(): void {
  if (globalTelemetry) {
    globalTelemetry.stopPeriodicFlush()
    globalTelemetry.reset()
  }
  globalTelemetry = null
}

/**
 * Create a telemetry collector with a specific configuration
 */
export function createTelemetryCollector(config?: TelemetryConfig): TelemetryCollector {
  return new TelemetryCollector(config)
}

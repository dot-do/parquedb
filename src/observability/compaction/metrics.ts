/**
 * Compaction Metrics Collection
 *
 * Utilities for collecting and emitting compaction metrics.
 * Supports multiple output formats:
 * - Structured console.log for Workers Analytics Engine
 * - In-memory time-series storage
 * - Export to Prometheus format
 */

import { logger } from '../../utils/logger'
import type {
  CompactionMetrics,
  MetricDataPoint,
  MetricTimeSeries,
  AggregatedMetrics,
  AnalyticsEngineDataPoint,
} from './types'

// =============================================================================
// In-Memory Metrics Store
// =============================================================================

/** Maximum number of data points to retain per metric series */
const MAX_DATA_POINTS = 1000

/** Metrics store - keyed by namespace, then by metric name */
const metricsStore: Map<string, Map<string, MetricDataPoint[]>> = new Map()

/** Last emitted metrics per namespace */
const lastMetrics: Map<string, CompactionMetrics> = new Map()

/**
 * Store a metric data point in memory
 */
function storeMetricPoint(
  namespace: string,
  metricName: string,
  value: number,
  timestamp: number
): void {
  let namespaceMetrics = metricsStore.get(namespace)
  if (!namespaceMetrics) {
    namespaceMetrics = new Map()
    metricsStore.set(namespace, namespaceMetrics)
  }

  let series = namespaceMetrics.get(metricName)
  if (!series) {
    series = []
    namespaceMetrics.set(metricName, series)
  }

  series.push({ timestamp, value })

  // Trim old data points
  if (series.length > MAX_DATA_POINTS) {
    series.splice(0, series.length - MAX_DATA_POINTS)
  }
}

// =============================================================================
// Metrics Emission
// =============================================================================

/**
 * Emit compaction metrics for a namespace
 *
 * This function is called from CompactionStateDO on each /update request.
 * It outputs structured logs that can be ingested by Workers Analytics Engine.
 *
 * @param metrics - The metrics to emit
 */
export function emitCompactionMetrics(metrics: CompactionMetrics): void {
  const timestamp = metrics.timestamp || Date.now()

  // Store in memory for dashboard queries
  lastMetrics.set(metrics.namespace, metrics)

  // Store each metric as a time-series data point
  const metricEntries: Array<[string, number]> = [
    ['windows_pending', metrics.windows_pending],
    ['windows_processing', metrics.windows_processing],
    ['windows_dispatched', metrics.windows_dispatched],
    ['files_pending', metrics.files_pending],
    ['oldest_window_age_ms', metrics.oldest_window_age_ms],
    ['known_writers', metrics.known_writers],
    ['active_writers', metrics.active_writers],
    ['bytes_pending', metrics.bytes_pending],
    ['windows_stuck', metrics.windows_stuck],
  ]

  for (const [name, value] of metricEntries) {
    storeMetricPoint(metrics.namespace, name, value, timestamp)
  }

  // Emit structured log for Workers Analytics Engine
  // Format: JSON with specific structure for analytics ingestion
  logger.info('compaction_metrics', {
    _source: 'compaction',
    namespace: metrics.namespace,
    timestamp,
    windows_pending: metrics.windows_pending,
    windows_processing: metrics.windows_processing,
    windows_dispatched: metrics.windows_dispatched,
    files_pending: metrics.files_pending,
    oldest_window_age_ms: metrics.oldest_window_age_ms,
    known_writers: metrics.known_writers,
    active_writers: metrics.active_writers,
    bytes_pending: metrics.bytes_pending,
    windows_stuck: metrics.windows_stuck,
  })
}

/**
 * Emit metrics to Cloudflare Analytics Engine
 *
 * @param analytics - The Analytics Engine binding
 * @param metrics - The metrics to emit
 */
export function emitToAnalyticsEngine(
  analytics: { writeDataPoint: (data: AnalyticsEngineDataPoint) => void },
  metrics: CompactionMetrics
): void {
  const timestamp = metrics.timestamp || Date.now()

  // Write individual metrics as data points
  // Analytics Engine supports up to 20 double values per data point
  analytics.writeDataPoint({
    indexes: [metrics.namespace],
    blobs: ['compaction_snapshot'],
    doubles: [
      metrics.windows_pending,
      metrics.windows_processing,
      metrics.windows_dispatched,
      metrics.files_pending,
      metrics.oldest_window_age_ms,
      metrics.known_writers,
      metrics.active_writers,
      metrics.bytes_pending,
      metrics.windows_stuck,
      timestamp,
    ],
  })
}

// =============================================================================
// Metrics Retrieval
// =============================================================================

/**
 * Get the latest metrics for a namespace
 */
export function getLatestMetrics(namespace: string): CompactionMetrics | undefined {
  return lastMetrics.get(namespace)
}

/**
 * Get the latest metrics for all namespaces
 */
export function getAllLatestMetrics(): Map<string, CompactionMetrics> {
  return new Map(lastMetrics)
}

/**
 * Get time-series data for a specific metric
 *
 * @param namespace - The namespace to query
 * @param metricName - The metric name
 * @param since - Optional timestamp to filter from
 * @param limit - Maximum number of points to return
 */
export function getMetricTimeSeries(
  namespace: string,
  metricName: string,
  since?: number,
  limit: number = 100
): MetricTimeSeries {
  const namespaceMetrics = metricsStore.get(namespace)
  let data: MetricDataPoint[] = []

  if (namespaceMetrics) {
    const series = namespaceMetrics.get(metricName)
    if (series) {
      data = since
        ? series.filter(p => p.timestamp >= since)
        : series.slice()

      // Apply limit (take most recent)
      if (data.length > limit) {
        data = data.slice(data.length - limit)
      }
    }
  }

  return {
    metric: metricName,
    namespace,
    data,
  }
}

/**
 * Get aggregated metrics across all namespaces
 */
export function getAggregatedMetrics(): AggregatedMetrics {
  const result: AggregatedMetrics = {
    total_windows_pending: 0,
    total_windows_processing: 0,
    total_windows_dispatched: 0,
    total_files_pending: 0,
    total_bytes_pending: 0,
    namespace_count: lastMetrics.size,
    by_namespace: {},
  }

  for (const [namespace, metrics] of Array.from(lastMetrics.entries())) {
    result.total_windows_pending += metrics.windows_pending
    result.total_windows_processing += metrics.windows_processing
    result.total_windows_dispatched += metrics.windows_dispatched
    result.total_files_pending += metrics.files_pending
    result.total_bytes_pending += metrics.bytes_pending
    result.by_namespace[namespace] = metrics
  }

  return result
}

/**
 * Clear all stored metrics (useful for testing)
 */
export function clearMetricsStore(): void {
  metricsStore.clear()
  lastMetrics.clear()
}

// =============================================================================
// Prometheus Format Export
// =============================================================================

/**
 * Export current metrics in Prometheus text format
 *
 * @param namespaces - Optional filter for specific namespaces
 */
export function exportPrometheusMetrics(namespaces?: string[]): string {
  const lines: string[] = []
  const timestamp = Date.now()

  // Filter namespaces if specified
  const metricsToExport = namespaces
    ? Array.from(lastMetrics.entries()).filter(([ns]) => namespaces.includes(ns))
    : Array.from(lastMetrics.entries())

  // Windows pending
  lines.push('# HELP parquedb_compaction_windows_pending Number of compaction windows pending')
  lines.push('# TYPE parquedb_compaction_windows_pending gauge')
  for (const [namespace, metrics] of metricsToExport) {
    lines.push(`parquedb_compaction_windows_pending{namespace="${namespace}"} ${metrics.windows_pending} ${timestamp}`)
  }

  // Windows processing
  lines.push('# HELP parquedb_compaction_windows_processing Number of compaction windows being processed')
  lines.push('# TYPE parquedb_compaction_windows_processing gauge')
  for (const [namespace, metrics] of metricsToExport) {
    lines.push(`parquedb_compaction_windows_processing{namespace="${namespace}"} ${metrics.windows_processing} ${timestamp}`)
  }

  // Windows dispatched
  lines.push('# HELP parquedb_compaction_windows_dispatched Number of compaction windows dispatched')
  lines.push('# TYPE parquedb_compaction_windows_dispatched gauge')
  for (const [namespace, metrics] of metricsToExport) {
    lines.push(`parquedb_compaction_windows_dispatched{namespace="${namespace}"} ${metrics.windows_dispatched} ${timestamp}`)
  }

  // Files pending
  lines.push('# HELP parquedb_compaction_files_pending Total files pending compaction')
  lines.push('# TYPE parquedb_compaction_files_pending gauge')
  for (const [namespace, metrics] of metricsToExport) {
    lines.push(`parquedb_compaction_files_pending{namespace="${namespace}"} ${metrics.files_pending} ${timestamp}`)
  }

  // Bytes pending
  lines.push('# HELP parquedb_compaction_bytes_pending Total bytes pending compaction')
  lines.push('# TYPE parquedb_compaction_bytes_pending gauge')
  for (const [namespace, metrics] of metricsToExport) {
    lines.push(`parquedb_compaction_bytes_pending{namespace="${namespace}"} ${metrics.bytes_pending} ${timestamp}`)
  }

  // Oldest window age (convert ms to seconds for Prometheus convention)
  lines.push('# HELP parquedb_compaction_oldest_window_age_seconds Age of oldest pending window in seconds')
  lines.push('# TYPE parquedb_compaction_oldest_window_age_seconds gauge')
  for (const [namespace, metrics] of metricsToExport) {
    const ageSeconds = Math.round(metrics.oldest_window_age_ms / 1000)
    lines.push(`parquedb_compaction_oldest_window_age_seconds{namespace="${namespace}"} ${ageSeconds} ${timestamp}`)
  }

  // Known writers
  lines.push('# HELP parquedb_compaction_known_writers Number of known writers')
  lines.push('# TYPE parquedb_compaction_known_writers gauge')
  for (const [namespace, metrics] of metricsToExport) {
    lines.push(`parquedb_compaction_known_writers{namespace="${namespace}"} ${metrics.known_writers} ${timestamp}`)
  }

  // Active writers
  lines.push('# HELP parquedb_compaction_active_writers Number of currently active writers')
  lines.push('# TYPE parquedb_compaction_active_writers gauge')
  for (const [namespace, metrics] of metricsToExport) {
    lines.push(`parquedb_compaction_active_writers{namespace="${namespace}"} ${metrics.active_writers} ${timestamp}`)
  }

  // Windows stuck
  lines.push('# HELP parquedb_compaction_windows_stuck Number of windows stuck in processing')
  lines.push('# TYPE parquedb_compaction_windows_stuck gauge')
  for (const [namespace, metrics] of metricsToExport) {
    lines.push(`parquedb_compaction_windows_stuck{namespace="${namespace}"} ${metrics.windows_stuck} ${timestamp}`)
  }

  return lines.join('\n')
}

// =============================================================================
// JSON Time-Series Export
// =============================================================================

/**
 * Export metrics as JSON time-series
 *
 * @param namespaces - Optional filter for specific namespaces
 * @param since - Optional timestamp to filter from
 * @param limit - Maximum data points per series
 */
export function exportJsonTimeSeries(
  namespaces?: string[],
  since?: number,
  limit: number = 100
): {
  timestamp: number
  namespaces: Record<string, {
    latest: CompactionMetrics | undefined
    timeSeries: Record<string, MetricTimeSeries>
  }>
} {
  const result: {
    timestamp: number
    namespaces: Record<string, {
      latest: CompactionMetrics | undefined
      timeSeries: Record<string, MetricTimeSeries>
    }>
  } = {
    timestamp: Date.now(),
    namespaces: {},
  }

  const metricNames = [
    'windows_pending',
    'windows_processing',
    'windows_dispatched',
    'files_pending',
    'oldest_window_age_ms',
    'known_writers',
    'active_writers',
    'bytes_pending',
    'windows_stuck',
  ]

  // Get all namespaces from both stores
  const allNamespaces = new Set([
    ...Array.from(metricsStore.keys()),
    ...Array.from(lastMetrics.keys()),
  ])

  for (const namespace of Array.from(allNamespaces)) {
    if (namespaces && !namespaces.includes(namespace)) {
      continue
    }

    const timeSeries: Record<string, MetricTimeSeries> = {}
    for (const metricName of metricNames) {
      timeSeries[metricName] = getMetricTimeSeries(namespace, metricName, since, limit)
    }

    result.namespaces[namespace] = {
      latest: lastMetrics.get(namespace),
      timeSeries,
    }
  }

  return result
}
